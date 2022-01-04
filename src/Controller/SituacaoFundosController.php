<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * SituacaoFundos Controller
 *
 * @property \App\Model\Table\SituacaoFundosTable $SituacaoFundos
 * @method \App\Model\Entity\SituacaoFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class SituacaoFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CnpjFundos', 'TipoSituacaoFundos'],
        ];
        $situacaoFundos = $this->paginate($this->SituacaoFundos);

        $this->set(compact('situacaoFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Situacao Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $situacaoFundo = $this->SituacaoFundos->get($id, [
            'contain' => ['CnpjFundos', 'TipoSituacaoFundos'],
        ]);

        $this->set(compact('situacaoFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $situacaoFundo = $this->SituacaoFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $situacaoFundo = $this->SituacaoFundos->patchEntity($situacaoFundo, $this->request->getData());
            if ($this->SituacaoFundos->save($situacaoFundo)) {
                $this->Flash->success(__('The situacao fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The situacao fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->SituacaoFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoSituacaoFundos = $this->SituacaoFundos->TipoSituacaoFundos->find('list', ['limit' => 200]);
        $this->set(compact('situacaoFundo', 'cnpjFundos', 'tipoSituacaoFundos'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Situacao Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $situacaoFundo = $this->SituacaoFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $situacaoFundo = $this->SituacaoFundos->patchEntity($situacaoFundo, $this->request->getData());
            if ($this->SituacaoFundos->save($situacaoFundo)) {
                $this->Flash->success(__('The situacao fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The situacao fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->SituacaoFundos->CnpjFundos->find('list', ['limit' => 200]);
        $tipoSituacaoFundos = $this->SituacaoFundos->TipoSituacaoFundos->find('list', ['limit' => 200]);
        $this->set(compact('situacaoFundo', 'cnpjFundos', 'tipoSituacaoFundos'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Situacao Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $situacaoFundo = $this->SituacaoFundos->get($id);
        if ($this->SituacaoFundos->delete($situacaoFundo)) {
            $this->Flash->success(__('The situacao fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The situacao fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
