<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * IndicadoresCarteiras Controller
 *
 * @property \App\Model\Table\IndicadoresCarteirasTable $IndicadoresCarteiras
 * @method \App\Model\Entity\IndicadoresCarteira[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class IndicadoresCarteirasController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CarteirasInvestimentos', 'TipoBenchmarks'],
        ];
        $indicadoresCarteiras = $this->paginate($this->IndicadoresCarteiras);

        $this->set(compact('indicadoresCarteiras'));
    }

    /**
     * View method
     *
     * @param string|null $id Indicadores Carteira id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $indicadoresCarteira = $this->IndicadoresCarteiras->get($id, [
            'contain' => ['CarteirasInvestimentos', 'TipoBenchmarks'],
        ]);

        $this->set(compact('indicadoresCarteira'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $indicadoresCarteira = $this->IndicadoresCarteiras->newEmptyEntity();
        if ($this->request->is('post')) {
            $indicadoresCarteira = $this->IndicadoresCarteiras->patchEntity($indicadoresCarteira, $this->request->getData());
            if ($this->IndicadoresCarteiras->save($indicadoresCarteira)) {
                $this->Flash->success(__('The indicadores carteira has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The indicadores carteira could not be saved. Please, try again.'));
        }
        $carteirasInvestimentos = $this->IndicadoresCarteiras->CarteirasInvestimentos->find('list', ['limit' => 200]);
        $tipoBenchmarks = $this->IndicadoresCarteiras->TipoBenchmarks->find('list', ['limit' => 200]);
        $this->set(compact('indicadoresCarteira', 'carteirasInvestimentos', 'tipoBenchmarks'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Indicadores Carteira id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $indicadoresCarteira = $this->IndicadoresCarteiras->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $indicadoresCarteira = $this->IndicadoresCarteiras->patchEntity($indicadoresCarteira, $this->request->getData());
            if ($this->IndicadoresCarteiras->save($indicadoresCarteira)) {
                $this->Flash->success(__('The indicadores carteira has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The indicadores carteira could not be saved. Please, try again.'));
        }
        $carteirasInvestimentos = $this->IndicadoresCarteiras->CarteirasInvestimentos->find('list', ['limit' => 200]);
        $tipoBenchmarks = $this->IndicadoresCarteiras->TipoBenchmarks->find('list', ['limit' => 200]);
        $this->set(compact('indicadoresCarteira', 'carteirasInvestimentos', 'tipoBenchmarks'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Indicadores Carteira id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $indicadoresCarteira = $this->IndicadoresCarteiras->get($id);
        if ($this->IndicadoresCarteiras->delete($indicadoresCarteira)) {
            $this->Flash->success(__('The indicadores carteira has been deleted.'));
        } else {
            $this->Flash->error(__('The indicadores carteira could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
