<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * CancelamentoFundos Controller
 *
 * @property \App\Model\Table\CancelamentoFundosTable $CancelamentoFundos
 * @method \App\Model\Entity\CancelamentoFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class CancelamentoFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $this->paginate = [
            'contain' => ['CnpjFundos'],
        ];
        $cancelamentoFundos = $this->paginate($this->CancelamentoFundos);

        $this->set(compact('cancelamentoFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Cancelamento Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $cancelamentoFundo = $this->CancelamentoFundos->get($id, [
            'contain' => ['CnpjFundos'],
        ]);

        $this->set(compact('cancelamentoFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $cancelamentoFundo = $this->CancelamentoFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $cancelamentoFundo = $this->CancelamentoFundos->patchEntity($cancelamentoFundo, $this->request->getData());
            if ($this->CancelamentoFundos->save($cancelamentoFundo)) {
                $this->Flash->success(__('The cancelamento fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The cancelamento fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->CancelamentoFundos->CnpjFundos->find('list', ['limit' => 200]);
        $this->set(compact('cancelamentoFundo', 'cnpjFundos'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Cancelamento Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $cancelamentoFundo = $this->CancelamentoFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $cancelamentoFundo = $this->CancelamentoFundos->patchEntity($cancelamentoFundo, $this->request->getData());
            if ($this->CancelamentoFundos->save($cancelamentoFundo)) {
                $this->Flash->success(__('The cancelamento fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The cancelamento fundo could not be saved. Please, try again.'));
        }
        $cnpjFundos = $this->CancelamentoFundos->CnpjFundos->find('list', ['limit' => 200]);
        $this->set(compact('cancelamentoFundo', 'cnpjFundos'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Cancelamento Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $cancelamentoFundo = $this->CancelamentoFundos->get($id);
        if ($this->CancelamentoFundos->delete($cancelamentoFundo)) {
            $this->Flash->success(__('The cancelamento fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The cancelamento fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
