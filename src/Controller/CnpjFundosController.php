<?php
declare(strict_types=1);

namespace App\Controller;

/**
 * CnpjFundos Controller
 *
 * @property \App\Model\Table\CnpjFundosTable $CnpjFundos
 * @method \App\Model\Entity\CnpjFundo[]|\Cake\Datasource\ResultSetInterface paginate($object = null, array $settings = [])
 */
class CnpjFundosController extends AppController
{
    /**
     * Index method
     *
     * @return \Cake\Http\Response|null|void Renders view
     */
    public function index()
    {
        $cnpjFundos = $this->paginate($this->CnpjFundos);

        $this->set(compact('cnpjFundos'));
    }

    /**
     * View method
     *
     * @param string|null $id Cnpj Fundo id.
     * @return \Cake\Http\Response|null|void Renders view
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function view($id = null)
    {
        $cnpjFundo = $this->CnpjFundos->get($id, [
            'contain' => ['CadastroFundos', 'CancelamentoFundos', 'DocExtratosFundos', 'DocInfDiarioFundos', 'IndicadoresFundos', 'OperacoesFinanceiras', 'SituacaoFundos'],
        ]);

        $this->set(compact('cnpjFundo'));
    }

    /**
     * Add method
     *
     * @return \Cake\Http\Response|null|void Redirects on successful add, renders view otherwise.
     */
    public function add()
    {
        $cnpjFundo = $this->CnpjFundos->newEmptyEntity();
        if ($this->request->is('post')) {
            $cnpjFundo = $this->CnpjFundos->patchEntity($cnpjFundo, $this->request->getData());
            if ($this->CnpjFundos->save($cnpjFundo)) {
                $this->Flash->success(__('The cnpj fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The cnpj fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('cnpjFundo'));
    }

    /**
     * Edit method
     *
     * @param string|null $id Cnpj Fundo id.
     * @return \Cake\Http\Response|null|void Redirects on successful edit, renders view otherwise.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function edit($id = null)
    {
        $cnpjFundo = $this->CnpjFundos->get($id, [
            'contain' => [],
        ]);
        if ($this->request->is(['patch', 'post', 'put'])) {
            $cnpjFundo = $this->CnpjFundos->patchEntity($cnpjFundo, $this->request->getData());
            if ($this->CnpjFundos->save($cnpjFundo)) {
                $this->Flash->success(__('The cnpj fundo has been saved.'));

                return $this->redirect(['action' => 'index']);
            }
            $this->Flash->error(__('The cnpj fundo could not be saved. Please, try again.'));
        }
        $this->set(compact('cnpjFundo'));
    }

    /**
     * Delete method
     *
     * @param string|null $id Cnpj Fundo id.
     * @return \Cake\Http\Response|null|void Redirects to index.
     * @throws \Cake\Datasource\Exception\RecordNotFoundException When record not found.
     */
    public function delete($id = null)
    {
        $this->request->allowMethod(['post', 'delete']);
        $cnpjFundo = $this->CnpjFundos->get($id);
        if ($this->CnpjFundos->delete($cnpjFundo)) {
            $this->Flash->success(__('The cnpj fundo has been deleted.'));
        } else {
            $this->Flash->error(__('The cnpj fundo could not be deleted. Please, try again.'));
        }

        return $this->redirect(['action' => 'index']);
    }
}
