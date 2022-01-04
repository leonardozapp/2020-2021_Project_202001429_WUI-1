<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * TipoSituacaoFundos Controller
 *
 * @property \App\Model\Table\TipoSituacaoFundosTable $TipoSituacaoFundos
 * @method \App\Model\Entity\TipoSituacaoFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class TipoSituacaoFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $tipoSituacaoFundos = $this->paginate($this->TipoSituacaoFundos);

        $this->set(compact('tipoSituacaoFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Tipo Situacao Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $tipoSituacaoFundo = $this->TipoSituacaoFundos->get($id, [
            'contain' => ['SituacaoFundos'],
        ]);

        $this->set(compact('tipoSituacaoFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $tipoSituacaoFundo = $this->TipoSituacaoFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $tipoSituacaoFundo = $this->TipoSituacaoFundos->patchEntity($tipoSituacaoFundo, $this->request->getData());
            if ($this->TipoSituacaoFundos->save($tipoSituacaoFundo)) {
                $this->Flash->success(__('The tipo situacao fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo situacao fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoSituacaoFundo'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Tipo Situacao Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $tipoSituacaoFundo = $this->TipoSituacaoFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $tipoSituacaoFundo = $this->TipoSituacaoFundos->patchEntity($tipoSituacaoFundo, $this->request->getData());
            if ($this->TipoSituacaoFundos->save($tipoSituacaoFundo)) {
                $this->Flash->success(__('The tipo situacao fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The tipo situacao fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('tipoSituacaoFundo'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Tipo Situacao Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $tipoSituacaoFundo = $this->TipoSituacaoFundos->get($id);
        if ($this->TipoSituacaoFundos->delete($tipoSituacaoFundo)) {
            $this->Flash->success(__('The tipo situacao fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The tipo situacao fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
